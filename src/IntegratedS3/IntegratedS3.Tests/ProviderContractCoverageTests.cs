using System.Reflection;
using System.Text.RegularExpressions;
using IntegratedS3.Abstractions.Services;
using IntegratedS3.Tests.Infrastructure;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// Ratchet on the provider contract harness (#268). Every <see cref="IStorageBackend"/> operation is called by
/// <c>StorageProviderContractTests</c> or listed in <see cref="NotYetCovered"/>. The list's length is pinned in
/// <see cref="NotYetCoveredCount"/> and may only go down: a new interface member arrives with a harness fact, and an
/// operation that gains one leaves the list, and lowers the count, in the same PR.
/// </summary>
public sealed class ProviderContractCoverageTests
{
    // 57 of the 86 operations on 2026-09-23. Lower it when an operation leaves the list; never raise it.
    private const int NotYetCoveredCount = 57;

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
    public void NotYetCovered_OnlyShrinks()
    {
        Assert.True(
            NotYetCovered.Length == NotYetCoveredCount,
            NotYetCovered.Length > NotYetCoveredCount
                ? $"NotYetCovered has {NotYetCovered.Length} entries, above its pinned {NotYetCoveredCount}. A new IStorageBackend operation arrives with a StorageProviderContractTests fact, never with a line in this list."
                : $"NotYetCovered has {NotYetCovered.Length} entries, below its pinned {NotYetCoveredCount}. Lower NotYetCoveredCount to {NotYetCovered.Length}.");
    }

    [Fact]
    public void BackendOperations_HaveNoOverloads()
    {
        // The harness scan identifies an operation by name, so an overload would count as covered without a fact.
        var overloaded = GetBackendMethods().GroupBy(static method => method.Name).Where(static group => group.Count() > 1).Select(static group => group.Key);

        Assert.Empty(overloaded);
    }

    [Fact]
    public void EveryBackendOperation_IsCalledByTheContractHarness_OrListedAsNotYetCovered()
    {
        var operations = GetBackendMethods().Select(static method => method.Name).ToHashSet(StringComparer.Ordinal);
        var called = GetOperationsCalledByTheHarness();

        var problems = new List<string>();
        AddProblem(problems, "IStorageBackend operations the harness does not call and NotYetCovered does not list (add a StorageProviderContractTests fact for each)",
            operations.Where(operation => !called.Contains(operation) && !NotYetCovered.Contains(operation)));
        AddProblem(problems, "operations the harness now calls that NotYetCovered still lists (remove them and lower NotYetCoveredCount)",
            NotYetCovered.Where(operation => operations.Contains(operation) && called.Contains(operation)));
        AddProblem(problems, "NotYetCovered entries that are not IStorageBackend operations (renamed, removed or misspelled)",
            NotYetCovered.Where(operation => !operations.Contains(operation)));

        Assert.True(problems.Count == 0, string.Join(" ", problems));
    }

    private static void AddProblem(List<string> problems, string description, IEnumerable<string> names)
    {
        var sorted = names.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        if (sorted.Length > 0) {
            problems.Add($"{description}: {string.Join(", ", sorted)}.");
        }
    }

    private static MethodInfo[] GetBackendMethods()
    {
        return typeof(IStorageBackend).GetInterfaces().Prepend(typeof(IStorageBackend))
            .SelectMany(static type => type.GetMethods(BindingFlags.Public | BindingFlags.Instance))
            .Where(static method => !method.IsSpecialName)
            .ToArray();
    }

    // ponytail: counts `storage.Name(` calls, where the harness binds `storage = fixture.Backend`, after stripping
    // comments and plain string literals (not verbatim or raw ones). A call through another variable is missed and
    // fails loudly. "Called" is not "asserted": most facts return early when the provider reports the capability
    // unsupported or has not opted in (#268). Parse the harness with Roslyn if that ever lets a gap through.
    private static HashSet<string> GetOperationsCalledByTheHarness()
    {
        var source = File.ReadAllText(RepositoryRoot.Combine("src", "IntegratedS3", "IntegratedS3.Testing", "StorageProviderContractTests.cs"));
        var code = Regex.Replace(source, @"""(?:[^""\\\n]|\\.)*""|//[^\n]*|/\*.*?\*/", static match => match.Value[0] == '"' ? "\"\"" : " ", RegexOptions.Singleline);
        return Regex.Matches(code, @"\bstorage\s*\.\s*(?<name>[A-Z][A-Za-z0-9]*)\s*[<(]")
            .Select(static match => match.Groups["name"].Value)
            .ToHashSet(StringComparer.Ordinal);
    }
}
