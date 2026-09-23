using System.Text.Json;
using System.Xml.Linq;
using IntegratedS3.Tests.Infrastructure;
using Xunit;

namespace IntegratedS3.Tests;

/// <summary>
/// The layering of the shipped packages, defined once (#265). Each packable project may reference exactly the
/// IntegratedS3 projects listed for it below. A new edge is a design decision: make it in this table, in the PR
/// that needs it. The tests read what restore computed (each project's obj/project.assets.json) rather than the
/// .csproj text, so a reference or package that arrives through a props file, an SDK or another package counts
/// like one written in the .csproj. A ProjectReference becomes a NuGet dependency of the package even when no type
/// from it is used.
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

    // The projects nuget-publish.yml packs: every <Project Path> of the solution, by project name.
    private static readonly Dictionary<string, string> SolutionProjects = XDocument
        .Load(RepositoryRoot.Combine("src", "IntegratedS3", "IntegratedS3.slnx"))
        .Descendants()
        .Where(static element => element.Name.LocalName == "Project")
        .Select(static element => RepositoryRoot.Combine("src", "IntegratedS3", (string)element.Attribute("Path")!))
        .ToDictionary(static csproj => Path.GetFileNameWithoutExtension(csproj), StringComparer.Ordinal);

    public static TheoryData<string> ShippedProjects => new(AllowedProjectReferences.Keys);

    public static TheoryData<string> CorePackageProjects => new(CorePackages);

    [Theory]
    [MemberData(nameof(ShippedProjects))]
    public void ShippedProject_ReferencesExactlyItsAllowedLayers(string project)
    {
        var actual = ReadAssets(project).GetProperty("project").GetProperty("restore").GetProperty("frameworks")
            .EnumerateObject()
            .SelectMany(static framework => framework.Value.TryGetProperty("projectReferences", out var references)
                ? references.EnumerateObject().Select(static reference => Path.GetFileNameWithoutExtension(reference.Name.Replace('\\', '/'))).ToArray()
                : [])
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal);

        Assert.Equal(AllowedProjectReferences[project].Order(StringComparer.Ordinal), actual);
    }

    // ponytail: reads IsPackable as written in each .csproj and ignores Condition attributes and SDK defaults; the
    // non-packable projects all say false explicitly today. Evaluate with MSBuild if one ever relies on a default.
    [Fact]
    public void EveryPackableProjectInTheSolution_HasARowInTheLayeringTable()
    {
        var packable = SolutionProjects
            .Where(static project => !XDocument.Load(project.Value).Descendants().Any(static element =>
                element.Name.LocalName == "IsPackable" && element.Value.Trim().Equals("false", StringComparison.OrdinalIgnoreCase)))
            .Select(static project => project.Key)
            .Order(StringComparer.Ordinal);

        Assert.Equal(AllowedProjectReferences.Keys.Order(StringComparer.Ordinal), packable);
    }

    [Theory]
    [MemberData(nameof(CorePackageProjects))]
    public void CorePackage_TakesNoEntityFrameworkAwsSdkOrAspNetCoreDependency(string project)
    {
        var assets = ReadAssets(project);
        // Every package restore resolved for the project, direct or transitive, its own framework references, and
        // the framework references its packages bring (a package can pull in Microsoft.AspNetCore.App).
        var packages = assets.GetProperty("libraries").EnumerateObject().Select(static library => library.Name.Split('/')[0]);
        var frameworks = assets.GetProperty("project").GetProperty("frameworks").EnumerateObject()
            .SelectMany(static framework => framework.Value.TryGetProperty("frameworkReferences", out var references)
                ? references.EnumerateObject().Select(static reference => reference.Name).ToArray()
                : []);
        var packageFrameworks = assets.GetProperty("targets").EnumerateObject()
            .SelectMany(static target => target.Value.EnumerateObject())
            .SelectMany(static library => library.Value.TryGetProperty("frameworkReferences", out var references)
                ? references.EnumerateArray().Select(static reference => reference.GetString()!).ToArray()
                : []);

        Assert.DoesNotContain(
            packages.Concat(frameworks).Concat(packageFrameworks),
            static dependency => DependencyPrefixesBannedInCorePackages.Any(prefix => dependency.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)));
    }

    private static JsonElement ReadAssets(string project)
    {
        var assetsPath = Path.Combine(Path.GetDirectoryName(SolutionProjects[project])!, "obj", "project.assets.json");
        using var assets = JsonDocument.Parse(File.ReadAllText(assetsPath));
        return assets.RootElement.Clone();
    }
}
