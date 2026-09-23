using System.Reflection;
using System.Text.RegularExpressions;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Tests.Infrastructure;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Ratchet on the provider contract harness (#268). Every <see cref="IStorageBackend"/> operation is called by
/// <c>StorageProviderContractTests</c> or listed in <see cref="NotYetCovered"/>, and the list may only shrink:
/// a new interface member arrives with a harness fact, and a member that gains one leaves the list in the same PR.
/// </summary>
public sealed class ProviderContractCoverageTests
{
    // 57 of the 86 operations on 2026-09-23. Remove a line when its harness fact lands; never add one.
    private static readonly string[] NotYetCovered =
    [
        "DeleteBucketAnalyticsConfigurationAsync",
        "DeleteBucketIntelligentTieringConfigurationAsync",
        "DeleteBucketInventoryConfigurationAsync",
        "DeleteBucketLifecycleAsync",
        "DeleteBucketMetricsConfigurationAsync",
        "DeleteBucketOwnershipControlsAsync",
        "DeleteBucketPublicAccessBlockAsync",
        "DeleteBucketReplicationAsync",
        "DeleteBucketTaggingAsync",
        "DeleteBucketWebsiteAsync",
        "GetBucketAccelerateAsync",
        "GetBucketAnalyticsConfigurationAsync",
        "GetBucketIntelligentTieringConfigurationAsync",
        "GetBucketInventoryConfigurationAsync",
        "GetBucketLifecycleAsync",
        "GetBucketLocationAsync",
        "GetBucketLoggingAsync",
        "GetBucketMetricsConfigurationAsync",
        "GetBucketNotificationConfigurationAsync",
        "GetBucketOwnershipControlsAsync",
        "GetBucketPublicAccessBlockAsync",
        "GetBucketReplicationAsync",
        "GetBucketRequestPaymentAsync",
        "GetBucketTaggingAsync",
        "GetBucketWebsiteAsync",
        "GetObjectAttributesAsync",
        "GetObjectLegalHoldAsync",
        "GetObjectLocationDescriptorAsync",
        "GetObjectLockConfigurationAsync",
        "GetObjectRetentionAsync",
        "GetProviderModeAsync",
        "ListBucketAnalyticsConfigurationsAsync",
        "ListBucketIntelligentTieringConfigurationsAsync",
        "ListBucketInventoryConfigurationsAsync",
        "ListBucketMetricsConfigurationsAsync",
        "ListMultipartUploadPartsAsync",
        "PresignObjectDirectAsync",
        "PutBucketAccelerateAsync",
        "PutBucketAnalyticsConfigurationAsync",
        "PutBucketIntelligentTieringConfigurationAsync",
        "PutBucketInventoryConfigurationAsync",
        "PutBucketLifecycleAsync",
        "PutBucketLoggingAsync",
        "PutBucketMetricsConfigurationAsync",
        "PutBucketNotificationConfigurationAsync",
        "PutBucketOwnershipControlsAsync",
        "PutBucketPublicAccessBlockAsync",
        "PutBucketReplicationAsync",
        "PutBucketRequestPaymentAsync",
        "PutBucketTaggingAsync",
        "PutBucketWebsiteAsync",
        "PutObjectLegalHoldAsync",
        "PutObjectLockConfigurationAsync",
        "PutObjectRetentionAsync",
        "RestoreObjectAsync",
        "SelectObjectContentAsync",
        "UploadPartCopyAsync",
    ];

    [Fact]
    public void EveryBackendOperation_IsCalledByTheContractHarness_OrListedAsNotYetCovered()
    {
        var calledByHarness = GetOperationsCalledByTheHarness();
        var uncovered = GetBackendOperations().Where(operation => !calledByHarness.Contains(operation)).ToHashSet(StringComparer.Ordinal);

        var newlyUncovered = uncovered.Except(NotYetCovered, StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        var nowCovered = NotYetCovered.Except(uncovered, StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();

        if (newlyUncovered.Length > 0 || nowCovered.Length > 0) {
            Assert.Fail(
                "IStorageBackend operations the contract harness does not call, and that are not in NotYetCovered: "
                + (newlyUncovered.Length > 0 ? string.Join(", ", newlyUncovered) : "none")
                + ". Add a StorageProviderContractTests fact for each."
                + " Operations the harness now calls that are still in NotYetCovered: "
                + (nowCovered.Length > 0 ? string.Join(", ", nowCovered) : "none")
                + ". Remove them from the list.");
        }
    }

    private static IEnumerable<string> GetBackendOperations()
    {
        return typeof(IStorageBackend).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(static method => !method.IsSpecialName)
            .Select(static method => method.Name)
            .Distinct(StringComparer.Ordinal);
    }

    // ponytail: matches call sites by method name, so a same-named call on another type counts as covered and
    // "called" is not "asserted". Parse the harness with Roslyn if that ever lets a gap through.
    private static HashSet<string> GetOperationsCalledByTheHarness()
    {
        var source = File.ReadAllText(RepositoryRoot.Combine("src", "IntegratedS3", "IntegratedS3.Testing", "StorageProviderContractTests.cs"));
        return Regex.Matches(source, @"\.(?<name>[A-Z][A-Za-z0-9]*)\s*[<(]")
            .Select(static match => match.Groups["name"].Value)
            .ToHashSet(StringComparer.Ordinal);
    }
}
