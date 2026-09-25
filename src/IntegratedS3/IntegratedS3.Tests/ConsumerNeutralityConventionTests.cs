using IntegratedS3.Tests.Infrastructure;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// IntegratedS3 serves every consumer through its generic contracts; what a consumer needs becomes a property
/// of those contracts, never a branch for that consumer (docs/distributed-architecture.md, "Consumers"). A
/// shipped source file that names a consumer is the first step of such a branch.
/// </summary>
public sealed class ConsumerNeutralityConventionTests
{
    private static readonly string[] ConsumerNames = ["PersonalS3", "Personal-S3", "Discord"];

    // Tests, benchmarks and sample hosts are not shipped.
    private static readonly HashSet<string> UnshippedProjects = new(StringComparer.OrdinalIgnoreCase)
    {
        "IntegratedS3.Tests", "IntegratedS3.E2E.Tests", "IntegratedS3.Benchmarks",
        "WebUi", "WebUi.MvcRazor", "WebUi.BlazorWasm", "WebUi.BlazorWasm.Client"
    };

    [Fact]
    public void ShippedSource_NamesNoConsumer()
    {
        var sourceRoot = RepositoryRoot.Combine("src", "IntegratedS3");
        var shippedFiles = Directory.EnumerateFiles(sourceRoot, "*", SearchOption.AllDirectories)
            .Where(path => IsShipped(Path.GetRelativePath(sourceRoot, path)))
            .ToList();

        // A scan that reads nothing must not pass.
        Assert.Contains(shippedFiles, static path => path.EndsWith(Path.Combine("IntegratedS3.Abstractions", "Services", "IStorageBackend.cs"), StringComparison.Ordinal));
        Assert.Contains(shippedFiles, static path => path.EndsWith(Path.Combine("IntegratedS3.Engine", "IntegratedS3.Engine.csproj"), StringComparison.Ordinal));

        var offenders = shippedFiles
            .SelectMany(path => File.ReadLines(path).Select((line, index) => (Path: Path.GetRelativePath(sourceRoot, path), Line: line, Number: index + 1)))
            .Where(static entry => ConsumerNames.Any(name => entry.Line.Contains(name, StringComparison.OrdinalIgnoreCase)))
            .Select(static entry => $"{entry.Path}:{entry.Number}: {entry.Line.Trim()}")
            .ToList();

        Assert.True(offenders.Count == 0, "Shipped source names a consumer; make the need a property of a generic contract instead:\n" + string.Join('\n', offenders));
    }

    private static bool IsShipped(string relativePath)
    {
        var segments = relativePath.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        return !(segments.Length > 1 && UnshippedProjects.Contains(segments[0]))
            && !segments.Contains("bin", StringComparer.OrdinalIgnoreCase)
            && !segments.Contains("obj", StringComparer.OrdinalIgnoreCase);
    }
}
