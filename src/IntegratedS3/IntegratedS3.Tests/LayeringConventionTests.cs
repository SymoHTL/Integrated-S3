using System.Xml.Linq;
using IntegratedS3.Tests.Infrastructure;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// The layering of the shipped packages, defined once (#265). Each packable project may reference exactly the
/// IntegratedS3 projects listed for it below. A new edge is a design decision: make it in this table, in the PR
/// that needs it. The table reads the declared references in each .csproj, because a ProjectReference becomes a
/// NuGet dependency of the package even when no type from it is used.
/// </summary>
public sealed class LayeringConventionTests
{
    private static readonly Dictionary<string, string[]> AllowedProjectReferences = new(StringComparer.Ordinal)
    {
        // Bottom: provider-agnostic contracts and the S3 wire protocol.
        ["IntegratedS3.Abstractions"] = [],
        ["IntegratedS3.Protocol"] = ["IntegratedS3.Abstractions"],
        // Orchestration above them, HTTP integration above orchestration.
        ["IntegratedS3.Core"] = ["IntegratedS3.Abstractions", "IntegratedS3.Protocol"],
        ["IntegratedS3.AspNetCore"] = ["IntegratedS3.Abstractions", "IntegratedS3.Protocol", "IntegratedS3.Core"],
        // Providers plug in through the bottom layer only.
        ["IntegratedS3.Provider.Disk"] = ["IntegratedS3.Abstractions", "IntegratedS3.Protocol"],
        ["IntegratedS3.Provider.S3"] = ["IntegratedS3.Abstractions", "IntegratedS3.Protocol"],
        // Optional integrations and helpers sit on Core; nothing below depends on them.
        ["IntegratedS3.EntityFramework"] = ["IntegratedS3.Core"],
        ["IntegratedS3.Client"] = ["IntegratedS3.Core", "IntegratedS3.Protocol"],
        ["IntegratedS3.Testing"] = ["IntegratedS3.Abstractions", "IntegratedS3.Protocol", "IntegratedS3.Core"],
    };

    // Every consumer takes these three, so they stay free of the optional integrations' dependencies.
    private static readonly string[] CorePackages = ["IntegratedS3.Abstractions", "IntegratedS3.Protocol", "IntegratedS3.Core"];

    private static readonly string[] DependencyPrefixesBannedInCorePackages = ["Microsoft.EntityFrameworkCore", "AWSSDK", "Microsoft.AspNetCore"];

    public static TheoryData<string> ShippedProjects => ToTheoryData(AllowedProjectReferences.Keys);

    public static TheoryData<string> CorePackageProjects => ToTheoryData(CorePackages);

    [Theory]
    [MemberData(nameof(ShippedProjects))]
    public void ShippedProject_ReferencesExactlyItsAllowedLayers(string project)
    {
        var actual = ReadReferences(project, "ProjectReference")
            .Select(static include => Path.GetFileNameWithoutExtension(include.Replace('\\', '/')))
            .Order(StringComparer.Ordinal);

        Assert.Equal(AllowedProjectReferences[project].Order(StringComparer.Ordinal), actual);
    }

    [Fact]
    public void EveryPackableProject_HasARowInTheLayeringTable()
    {
        var packable = Directory.EnumerateDirectories(RepositoryRoot.Combine("src", "IntegratedS3"))
            .Select(static directory => Path.Combine(directory, Path.GetFileName(directory) + ".csproj"))
            .Where(File.Exists)
            .Where(static csproj => !XDocument.Load(csproj).Descendants("IsPackable").Any(static element => element.Value.Trim() == "false"))
            .Select(static csproj => Path.GetFileNameWithoutExtension(csproj))
            .Order(StringComparer.Ordinal);

        Assert.Equal(AllowedProjectReferences.Keys.Order(StringComparer.Ordinal), packable);
    }

    [Theory]
    [MemberData(nameof(CorePackageProjects))]
    public void CorePackage_TakesNoEntityFrameworkAwsSdkOrAspNetCoreDependency(string project)
    {
        var dependencies = ReadReferences(project, "PackageReference").Concat(ReadReferences(project, "FrameworkReference"));

        Assert.DoesNotContain(
            dependencies,
            static dependency => DependencyPrefixesBannedInCorePackages.Any(prefix => dependency.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)));
    }

    private static IEnumerable<string> ReadReferences(string project, string itemType)
    {
        return XDocument.Load(RepositoryRoot.Combine("src", "IntegratedS3", project, project + ".csproj"))
            .Descendants(itemType)
            .Select(static element => (string?)element.Attribute("Include"))
            .OfType<string>();
    }

    private static TheoryData<string> ToTheoryData(IEnumerable<string> values)
    {
        var data = new TheoryData<string>();
        foreach (var value in values) {
            data.Add(value);
        }

        return data;
    }
}
