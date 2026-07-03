using System.Security.Claims;
using IntegratedS3.Abstractions.Errors;
using IntegratedS3.Abstractions.Results;
using IntegratedS3.Core.Models;
using IntegratedS3.Core.Options;
using Microsoft.Extensions.Options;

namespace IntegratedS3.Core.Services;

/// <summary>
/// An <see cref="IIntegratedS3AuthorizationService"/> that enforces the <c>scope</c> claims promoted from a
/// credential's <c>Scopes</c> onto each storage operation. Opt in via
/// <see cref="DependencyInjection.IntegratedS3CoreServiceCollectionExtensions.AddIntegratedS3ScopeBasedAuthorization(Microsoft.Extensions.DependencyInjection.IServiceCollection, System.Action{ScopeBasedIntegratedS3AuthorizationOptions}?)"/>.
/// </summary>
/// <remarks>
/// See <see cref="ScopeBasedIntegratedS3AuthorizationOptions"/> for the scope grammar. When a principal carries
/// multiple scopes the operation is permitted if any single scope grants it; otherwise the request is denied with
/// <see cref="StorageErrorCode.AccessDenied"/> and a suggested HTTP <c>403</c> status.
/// </remarks>
internal sealed class ScopeBasedIntegratedS3AuthorizationService(
    IOptions<ScopeBasedIntegratedS3AuthorizationOptions> options) : IIntegratedS3AuthorizationService
{
    private const string ScopeClaimType = "scope";
    private const string AdminScope = "admin";
    private const string WildcardScope = "*";
    private const string ReadScope = "read";
    private const string WriteScope = "write";
    private const string BucketScopePrefix = "bucket:";

    public ValueTask<StorageResult> AuthorizeAsync(ClaimsPrincipal principal, StorageAuthorizationRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(principal);
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var isRead = IsReadOperation(request.Operation);

        var hasAnyScope = false;
        foreach (var claim in principal.Claims) {
            if (!string.Equals(claim.Type, ScopeClaimType, StringComparison.Ordinal)) {
                continue;
            }

            var scope = claim.Value?.Trim();
            if (string.IsNullOrEmpty(scope)) {
                continue;
            }

            hasAnyScope = true;
            if (ScopeGrantsOperation(scope, isRead, request.BucketName)) {
                return ValueTask.FromResult(StorageResult.Success());
            }
        }

        if (!hasAnyScope && options.Value.AllowUnscopedPrincipals) {
            return ValueTask.FromResult(StorageResult.Success());
        }

        return ValueTask.FromResult(StorageResult.Failure(new StorageError
        {
            Code = StorageErrorCode.AccessDenied,
            Message = hasAnyScope
                ? $"The credential's scopes do not grant '{request.Operation}'"
                    + (request.BucketName is null ? "." : $" on bucket '{request.BucketName}'.")
                : "The credential has no scopes granting the requested operation.",
            BucketName = request.BucketName,
            ObjectKey = request.Key,
            VersionId = request.VersionId,
            SuggestedHttpStatusCode = 403
        }));
    }

    private static bool ScopeGrantsOperation(string scope, bool isReadOperation, string? bucketName)
    {
        if (string.Equals(scope, AdminScope, StringComparison.OrdinalIgnoreCase)
            || string.Equals(scope, WildcardScope, StringComparison.Ordinal)) {
            return true;
        }

        if (string.Equals(scope, ReadScope, StringComparison.OrdinalIgnoreCase)) {
            return isReadOperation;
        }

        if (string.Equals(scope, WriteScope, StringComparison.OrdinalIgnoreCase)) {
            return !isReadOperation;
        }

        if (scope.StartsWith(BucketScopePrefix, StringComparison.OrdinalIgnoreCase)) {
            return BucketScopeGrantsOperation(scope[BucketScopePrefix.Length..], isReadOperation, bucketName);
        }

        return false;
    }

    private static bool BucketScopeGrantsOperation(string remainder, bool isReadOperation, string? bucketName)
    {
        // Bucket-scoped grants never apply to service-level operations (no target bucket).
        if (string.IsNullOrEmpty(bucketName)) {
            return false;
        }

        var separatorIndex = remainder.LastIndexOf(':');
        string scopedBucket;
        string? qualifier = null;
        if (separatorIndex >= 0) {
            var candidateQualifier = remainder[(separatorIndex + 1)..];
            if (string.Equals(candidateQualifier, ReadScope, StringComparison.OrdinalIgnoreCase)
                || string.Equals(candidateQualifier, WriteScope, StringComparison.OrdinalIgnoreCase)) {
                qualifier = candidateQualifier;
                scopedBucket = remainder[..separatorIndex];
            }
            else {
                // A ':' that is not a recognized qualifier is treated as part of the bucket name.
                scopedBucket = remainder;
            }
        }
        else {
            scopedBucket = remainder;
        }

        if (!string.Equals(scopedBucket, bucketName, StringComparison.Ordinal)) {
            return false;
        }

        if (qualifier is null) {
            return true;
        }

        return string.Equals(qualifier, ReadScope, StringComparison.OrdinalIgnoreCase)
            ? isReadOperation
            : !isReadOperation;
    }

    /// <summary>
    /// Classifies an operation as read-class (listing, reading, or metadata retrieval) or write-class.
    /// Unknown/future operations default to write-class so they require the more privileged scope.
    /// </summary>
    private static bool IsReadOperation(StorageOperationType operation) => operation switch
    {
        StorageOperationType.ListBuckets => true,
        StorageOperationType.HeadBucket => true,
        StorageOperationType.GetBucketAcl => true,
        StorageOperationType.GetBucketPolicy => true,
        StorageOperationType.GetBucketLocation => true,
        StorageOperationType.GetBucketVersioning => true,
        StorageOperationType.GetBucketCors => true,
        StorageOperationType.GetBucketDefaultEncryption => true,
        StorageOperationType.ListObjects => true,
        StorageOperationType.ListObjectVersions => true,
        StorageOperationType.ListMultipartUploads => true,
        StorageOperationType.ListMultipartParts => true,
        StorageOperationType.GetObject => true,
        StorageOperationType.HeadObject => true,
        StorageOperationType.GetObjectAcl => true,
        StorageOperationType.GetObjectTags => true,
        StorageOperationType.PresignGetObject => true,
        StorageOperationType.PresignHeadObject => true,
        StorageOperationType.GetBucketTagging => true,
        StorageOperationType.GetBucketLogging => true,
        StorageOperationType.GetBucketWebsite => true,
        StorageOperationType.GetBucketRequestPayment => true,
        StorageOperationType.GetBucketAccelerate => true,
        StorageOperationType.GetBucketLifecycle => true,
        StorageOperationType.GetBucketReplication => true,
        StorageOperationType.GetBucketNotificationConfiguration => true,
        StorageOperationType.GetObjectLockConfiguration => true,
        StorageOperationType.GetBucketAnalyticsConfiguration => true,
        StorageOperationType.GetBucketMetricsConfiguration => true,
        StorageOperationType.GetBucketInventoryConfiguration => true,
        StorageOperationType.GetBucketIntelligentTieringConfiguration => true,
        StorageOperationType.ListBucketAnalyticsConfigurations => true,
        StorageOperationType.ListBucketMetricsConfigurations => true,
        StorageOperationType.ListBucketInventoryConfigurations => true,
        StorageOperationType.ListBucketIntelligentTieringConfigurations => true,
        StorageOperationType.SelectObjectContent => true,
        StorageOperationType.GetObjectAttributes => true,
        _ => false
    };
}
